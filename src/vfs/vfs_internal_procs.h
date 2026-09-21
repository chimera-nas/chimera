// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stddef.h>
#include "vfs.h"
#include "vfs_kv.h"

struct evpl_iovec;

typedef void (*chimera_vfs_lookup_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

void
chimera_vfs_lookup_at(
    struct chimera_vfs_thread       *vfs,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    uint64_t                         attr_mask,
    uint64_t                         dir_attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data);

void
chimera_vfs_lookup(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    uint64_t                       attr_mask,
    uint32_t                       flags,
    chimera_vfs_lookup_callback_t  callback,
    void                          *private_data);


void
chimera_vfs_create(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_create_callback_t  callback,
    void                          *private_data);

/* Path-based operations */

void
chimera_vfs_open(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_open_callback_t    callback,
    void                          *private_data);

void
chimera_vfs_mkdir(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_mkdir_callback_t   callback,
    void                          *private_data);

void
chimera_vfs_remove(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    unsigned int                   flags,
    chimera_vfs_remove_callback_t  callback,
    void                          *private_data);

void
chimera_vfs_rename(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    const char                    *new_path,
    int                            new_pathlen,
    chimera_vfs_rename_callback_t  callback,
    void                          *private_data);

void
chimera_vfs_symlink(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    const char                    *target,
    int                            targetlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_symlink_callback_t callback,
    void                          *private_data);

void
chimera_vfs_link(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *old_path,
    int                            old_pathlen,
    /* CHIMERA_VFS_LOOKUP_FOLLOW resolves a final-component symlink in
     * old_path and links its target (linkat AT_SYMLINK_FOLLOW); 0 links
     * the symlink itself. */
    unsigned int                   source_lookup_flags,
    const char                    *new_path,
    int                            new_pathlen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    chimera_vfs_link_callback_t    callback,
    void                          *private_data);

void
chimera_vfs_mknod(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const char                    *path,
    int                            pathlen,
    struct chimera_vfs_attrs      *set_attr,
    uint64_t                       attr_mask,
    chimera_vfs_mknod_callback_t   callback,
    void                          *private_data);

void
chimera_vfs_find(
    struct chimera_vfs_thread     *vfs,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       attr_mask,
    chimera_vfs_filter_callback_t  filter,
    chimera_vfs_find_callback_t    callback,
    chimera_vfs_find_complete_t    complete,
    void                          *private_data);



typedef void (*chimera_vfs_getattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

/* Recall every OTHER caching lease on the file backing `handle` (the operating
 * open's own lease is spared) and PARK until the recall drains, then invoke
 * `callback`.  A namespace-mutation recall (breaks a peer's handle cache) with no
 * backend op -- used by the SMB delete-on-close path so the peer's lease break is
 * acked before the SetInfo reply is sent (smb2.lease.unlink). */
typedef void (*chimera_vfs_recall_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

void
chimera_vfs_recall_handle_lease(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    chimera_vfs_recall_callback_t   callback,
    void                           *private_data);

/* Completion for chimera_vfs_recall_caching_fh: `still_open` reports whether the
 * file still has a live (non-implicit) share holder once the recall has drained
 * (a holder that did NOT close in response to the handle-lease break). */
typedef void (*chimera_vfs_recall_fh_callback_t)(
    enum chimera_vfs_error error_code,
    int                    still_open,
    void                  *private_data);

/* Single-step recall of every caching lease on the file named by a bare FH
 * (no open handle is spared), breaking each holder's handle cache once
 * (RH -> R) and PARKing until the recall drains, then report whether a holder
 * kept the file open.  Used by the SMB directory-rename path to break the
 * handle leases of files open inside a directory being renamed. */
void
chimera_vfs_recall_caching_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    uint32_t                         fh_len,
    chimera_vfs_recall_fh_callback_t callback,
    void                            *private_data);

void
chimera_vfs_getattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    chimera_vfs_getattr_callback_t  callback,
    void                           *private_data);

typedef void (*chimera_vfs_setattr_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

/* Internal metadata completion of an already authorized data mutation.
 * Skips authorization, recall, and pNFS redirection; never use for client SETATTR. */
void chimera_vfs_setattr_after_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

void
chimera_vfs_setattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

/* Descriptor-originated variant: WRITE_DATA-only mutations (ftruncate,
 * futimens-to-now) are authorized by the handle's open-time access grant
 * rather than the file's current mode (POSIX rights retention). */
void
chimera_vfs_fsetattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data);

/* Descriptor mutation with an explicit cache owner; the actor is copied before
* return and exempts that owner and its shared lease from namespace recalls. */
void
chimera_vfs_fsetattr_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_setattr_callback_t    callback,
    void                             *private_data,
    const struct chimera_claim_actor *actor);

void
chimera_vfs_readdir(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        attr_mask,
    uint64_t                        dir_attr_mask,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        flags,
    const char                     *match_pattern,
    int                             match_pattern_len,
    chimera_vfs_readdir_callback_t  callback,
    chimera_vfs_readdir_complete_t  complete,
    void                           *private_data);

/* SMB-style directory wildcard match (MS-FSA 2.1.4.4), exposed for callers that
 * filter outside chimera_vfs_readdir.  A NULL/empty pattern matches everything. */
int
chimera_vfs_dirent_match(
    const char *name,
    int         namelen,
    const char *pattern,
    int         patternlen);

typedef void (*chimera_vfs_open_fh_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

void
chimera_vfs_open_fh(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    unsigned int                   flags,
    chimera_vfs_open_fh_callback_t callback,
    void                          *private_data);

/* Variant that persists an opaque handle-state record atomically with the
 * open (backends advertising CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE); handle_state
 * may be NULL, in which case it behaves exactly like chimera_vfs_open_fh. */
void
chimera_vfs_open_fh_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    unsigned int                     flags,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_fh_callback_t   callback,
    void                            *private_data);

typedef void (*chimera_vfs_open_at_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data);

void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data);

/* Variant that persists an opaque handle-state record atomically with the
 * open (backends advertising CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE); handle_state
 * may be NULL, in which case it behaves exactly like chimera_vfs_open_at. */
void
chimera_vfs_open_at_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    unsigned int                     flags,
    struct chimera_vfs_attrs        *attr,
    uint64_t                         attr_mask,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_at_callback_t   callback,
    void                            *private_data);


typedef void (*chimera_vfs_create_unlinked_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

void
chimera_vfs_create_unlinked(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const uint8_t                         *fh,
    int                                    fh_len,
    struct chimera_vfs_attrs              *attr,
    uint64_t                               attr_mask,
    chimera_vfs_create_unlinked_callback_t callback,
    void                                  *private_data);

/*
 * Release an open handle returned by chimera_vfs_open_fh()/open_at().  This is
 * a non-inline export of the internal inline chimera_vfs_release(); out-of-tree
 * consumers that link libchimera_vfs (e.g. in-process VFS module tests) use it
 * to avoid pulling in the internal open-cache headers.
 */
void
chimera_vfs_release_handle(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle);

typedef void (*chimera_vfs_mkdir_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_mkdir_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_at_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_mknod_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_mknod_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mknod_at_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_remove_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_remove_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data);

/* Inode-scoped remove (only unlinks the name while it still resolves to
 * child_fh).  Same signature as chimera_vfs_remove_at; see its definition. */
void
chimera_vfs_remove_at_match_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    int                              namelen,
    const uint8_t                   *child_fh,
    int                              child_fh_len,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    chimera_vfs_remove_at_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_read_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *iov,
    int                             niov,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data);

/* As chimera_vfs_read(), but attributes the I/O to `io_owner` (a lease-
 * holding client's owner) so its own delegation/oplock is not recalled by
 * its own read.  Pass NULL to have chimera hold an implicit lease on behalf
 * of a leaseless actor (equivalent to chimera_vfs_read()). */
void
chimera_vfs_read_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               niov,
    uint64_t                          attrmask,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_read_callback_t       callback,
    void                             *private_data);

/* As chimera_vfs_read(), but the caller supplies its own destination buffers
 * (dest_iov/dest_niov) for the data to land in.  work_iov/work_niov is scratch
 * the core/backend reads through; on completion the data is guaranteed to be in
 * dest_iov (zero-copy where a backend can land it there directly, a scatter-
 * copy otherwise).  The caller retains ownership of dest_iov (borrow): it must
 * keep the buffers alive until the callback and release them afterwards.  The
 * callback's iov/niov reference dest_iov. */
void
chimera_vfs_read_into(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *work_iov,
    int                             work_niov,
    struct evpl_iovec              *dest_iov,
    int                             dest_niov,
    uint64_t                        attrmask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data);

typedef void (*chimera_vfs_write_callback_t)(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data);

/* As chimera_vfs_write(), but attributes the I/O to `io_owner` (a lease-
 * holding client's owner) so its own write delegation/oplock is not recalled
 * by its own write, while other holders' read caches are still invalidated.
 * Pass NULL to have chimera hold an implicit lease on behalf of a leaseless
 * actor (equivalent to chimera_vfs_write()). */
void
chimera_vfs_write_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner,
    chimera_vfs_write_callback_t      callback,
    void                             *private_data);

typedef void (*chimera_vfs_commit_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_commit(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        count,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_commit_callback_t   callback,
    void                           *private_data);

/* pNFS: ask a layout-sourcing backend (CHIMERA_VFS_CAP_LAYOUT_SOURCE) where a
 * file's data lives.  segments/devices are valid only while the callback runs. */
typedef void (*chimera_vfs_get_layout_callback_t)(
    enum chimera_vfs_error                   error_code,
    uint32_t                                 layout_class,
    uint32_t                                 num_segments,
    const struct chimera_vfs_layout_segment *segments,
    uint32_t                                 num_devices,
    const struct chimera_vfs_layout_device  *devices,
    void                                    *private_data);

void
chimera_vfs_get_layout(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    uint32_t                          iomode,
    uint32_t                          layout_class,
    uint32_t                          max_segments,
    chimera_vfs_get_layout_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_symlink_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data);

void
chimera_vfs_symlink_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const char                       *target,
    int                               targetlen,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_symlink_at_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_readlink_callback_t)(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

void
chimera_vfs_readlink(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    void                           *target,
    uint32_t                        target_maxlength,
    uint64_t                        attr_mask,
    chimera_vfs_readlink_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_rename_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data);

void
chimera_vfs_rename_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    const char                      *name,
    int                              namelen,
    const void                      *new_fh,
    int                              new_fhlen,
    const char                      *new_name,
    int                              new_namelen,
    const uint8_t                   *target_fh,
    int                              target_fh_len,
    unsigned int                     flags,
    uint64_t                         pre_attr_mask,
    uint64_t                         post_attr_mask,
    const uint8_t                   *parent_lease_skip,
    struct chimera_vfs_open_handle  *op_handle,
    chimera_vfs_rename_at_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_link_at_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data);

void
chimera_vfs_link_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    const void                     *fh,
    int                             fhlen,
    const void                     *dir_fh,
    int                             dir_fhlen,
    const char                     *name,
    int                             namelen,
    unsigned int                    replace,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    const uint8_t                  *parent_lease_skip,
    struct chimera_vfs_open_handle *op_handle,
    chimera_vfs_link_at_callback_t  callback,
    void                           *private_data);

typedef void (*chimera_vfs_allocate_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_allocate(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_allocate_callback_t callback,
    void                           *private_data);

typedef void (*chimera_vfs_copy_range_callback_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_clone_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_clone_range(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *src_handle,
    uint64_t                           src_offset,
    struct chimera_vfs_open_handle    *dst_handle,
    uint64_t                           dst_offset,
    uint64_t                           length,
    uint64_t                           pre_attr_mask,
    uint64_t                           post_attr_mask,
    chimera_vfs_clone_range_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_read_plus_callback_t)(
    enum chimera_vfs_error error_code,
    uint32_t               is_data,
    uint64_t               length,
    uint32_t               eof,
    void                  *private_data);

void
chimera_vfs_read_plus(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    uint64_t                         offset,
    uint64_t                         length,
    chimera_vfs_read_plus_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_write_same_callback_t)(
    enum chimera_vfs_error    error_code,
    uint64_t                  count,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data);

void
chimera_vfs_write_same(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          block_size,
    uint64_t                          block_count,
    const void                       *pattern,
    uint32_t                          pattern_len,
    uint32_t                          reloff_pattern,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_write_same_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_move_range_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *src_post_attr,
    struct chimera_vfs_attrs *dst_pre_attr,
    struct chimera_vfs_attrs *dst_post_attr,
    void                     *private_data);

void
chimera_vfs_move_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          src_post_attr_mask,
    uint64_t                          dst_pre_attr_mask,
    uint64_t                          dst_post_attr_mask,
    chimera_vfs_move_range_callback_t callback,
    void                             *private_data);

typedef void (*chimera_vfs_seek_callback_t)(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data);

void
chimera_vfs_seek(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        what,
    chimera_vfs_seek_callback_t     callback,
    void                           *private_data);

typedef void (*chimera_vfs_getparent_callback_t)(
    enum chimera_vfs_error error_code,
    const uint8_t         *parent_fh,
    uint16_t               parent_fh_len,
    const char            *name,
    uint16_t               name_len,
    void                  *private_data);

void
chimera_vfs_getparent(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    chimera_vfs_getparent_callback_t callback,
    void                            *private_data);

/*
 * RFC 8276 extended attribute operations.
 */

typedef void (*chimera_vfs_get_xattr_callback_t)(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data);

void
chimera_vfs_get_xattr(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    void                            *value,
    uint32_t                         value_maxlen,
    chimera_vfs_get_xattr_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_set_xattr_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

void
chimera_vfs_set_xattr(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    uint32_t                         option,
    const char                      *name,
    uint32_t                         namelen,
    const void                      *value,
    uint32_t                         value_len,
    chimera_vfs_set_xattr_callback_t callback,
    void                            *private_data);

typedef void (*chimera_vfs_list_xattrs_callback_t)(
    enum chimera_vfs_error error_code,
    const char            *names,    /* back-to-back NUL-terminated names */
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data);

void
chimera_vfs_list_xattrs(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *handle,
    uint64_t                           cookie,
    void                              *buffer,
    uint32_t                           max_bytes,
    chimera_vfs_list_xattrs_callback_t callback,
    void                              *private_data);

typedef void (*chimera_vfs_remove_xattr_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

void
chimera_vfs_remove_xattr(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    const char                         *name,
    uint32_t                            namelen,
    chimera_vfs_remove_xattr_callback_t callback,
    void                               *private_data);

/*
 * Named-stream (SMB Alternate Data Stream) operations.
 * Gated by CHIMERA_VFS_CAP_NAMED_STREAMS.
 */

/* Open (and optionally create/truncate) a named data fork on the base file
 * referenced by `handle`.  On success `oh` is a VFS open handle for the
 * stream: read/write/getattr/setattr against it operate on the stream's own
 * data and size, while metadata (mode/owner/timestamps) mirror the base file.
 * `set_attr` may be NULL.  Stream-open `flags` use CHIMERA_VFS_OPEN_* (CREATE/
 * EXCLUSIVE/TRUNCATE) with the same semantics as chimera_vfs_open_at. */
typedef void (*chimera_vfs_open_stream_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

void
chimera_vfs_open_stream(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *handle,
    const char                        *name,
    uint32_t                           namelen,
    uint32_t                           flags,
    struct chimera_vfs_attrs          *set_attr,
    uint64_t                           attr_mask,
    chimera_vfs_open_stream_callback_t callback,
    void                              *private_data);

/* Enumerate the named streams of the base file referenced by `handle`.  The
 * buffer is filled with packed struct chimera_vfs_stream_entry records (each
 * followed by name_len name bytes); the default unnamed data fork is reported
 * first as an entry with an empty name and the file's size. */
typedef void (*chimera_vfs_list_streams_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *records,  /* packed chimera_vfs_stream_entry + names */
    uint32_t               records_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data);

void
chimera_vfs_list_streams(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    uint64_t                            cookie,
    void                               *buffer,
    uint32_t                            max_bytes,
    int                                 want_fh,
    chimera_vfs_list_streams_callback_t callback,
    void                               *private_data);

/* Remove a single named stream from the base file referenced by `handle`. */
typedef void (*chimera_vfs_remove_stream_callback_t)(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data);

void
chimera_vfs_remove_stream(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    chimera_vfs_remove_stream_callback_t callback,
    void                                *private_data);
