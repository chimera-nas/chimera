// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_state.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_link_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_link_at_callback_t callback   = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(thread, name_cache,
                                      request->link_at.dir_fh_hash,
                                      request->link_at.dir_fh,
                                      request->link_at.dir_fhlen,
                                      request->link_at.name_hash,
                                      request->link_at.name,
                                      request->link_at.namelen,
                                      request->fh,
                                      request->fh_len);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->link_at.dir_fh_hash,
                                      request->link_at.dir_fh,
                                      request->link_at.dir_fhlen,
                                      &request->link_at.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->link_at.r_attr);

        if (request->link_at.r_replaced_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_attr_cache_insert(thread, attr_cache,
                                          request->link_at.r_replaced_attr.va_fh_hash,
                                          request->link_at.r_replaced_attr.va_fh,
                                          request->link_at.r_replaced_attr.va_fh_len,
                                          &request->link_at.r_replaced_attr);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->link_at.r_attr,
             &request->link_at.r_dir_pre_attr,
             &request->link_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_link_at_complete */

static void
chimera_vfs_link_at_dispatch(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *dir_fh,
    int                            dir_fhlen,
    const char                    *name,
    int                            namelen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_link_at_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                              = CHIMERA_VFS_OP_LINK_AT;
    request->complete                            = chimera_vfs_link_at_complete;
    request->link_at.dir_fh_hash                 = chimera_vfs_hash(dir_fh, dir_fhlen);
    request->link_at.dir_fh                      = dir_fh;
    request->link_at.dir_fhlen                   = dir_fhlen;
    request->link_at.name                        = name;
    request->link_at.namelen                     = namelen;
    request->link_at.name_hash                   = chimera_vfs_hash(name, namelen);
    request->link_at.replace                     = replace;
    request->link_at.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link_at.r_attr.va_set_mask          = 0;
    request->link_at.r_replaced_attr.va_req_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link_at.r_replaced_attr.va_set_mask = 0;
    request->link_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->link_at.r_dir_pre_attr.va_set_mask  = 0;
    request->link_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_FH |
        CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    /* RFC 7530 §10.4.5: adding a hard link to a delegated file must recall the
     * delegation first.  request->fh is the source file being linked. */
    chimera_vfs_io_recall(request, request->fh, request->fh_len,
                          request->fh_hash, 0 /* namespace recall: revoke fully */,
                          chimera_vfs_dispatch);
} /* chimera_vfs_link_at_dispatch */

/*
 * Enforcement pre-step context: hard-linking an existing object into a
 * directory requires ADD_FILE on that directory.
 */
struct chimera_vfs_link_at_gate {
    struct chimera_vfs_thread     *thread;
    const struct chimera_vfs_cred *cred;
    const void                    *fh;
    int                            fhlen;
    const void                    *dir_fh;
    int                            dir_fhlen;
    const char                    *name;
    int                            namelen;
    unsigned int                   replace;
    uint64_t                       attr_mask;
    uint64_t                       pre_attr_mask;
    uint64_t                       post_attr_mask;
    chimera_vfs_link_at_callback_t callback;
    void                          *private_data;
};

static void
chimera_vfs_link_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_link_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_link_at_dispatch(gate->thread, gate->cred, gate->fh, gate->fhlen,
                                 gate->dir_fh, gate->dir_fhlen, gate->name,
                                 gate->namelen, gate->replace, gate->attr_mask,
                                 gate->pre_attr_mask, gate->post_attr_mask,
                                 gate->callback, gate->private_data);
    free(gate);
} /* chimera_vfs_link_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_link_at(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *dir_fh,
    int                            dir_fhlen,
    const char                    *name,
    int                            namelen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_link_at_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_module       *module;
    struct chimera_vfs_link_at_gate *gate;

    if (namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, private_data);
        return;
    }

    module = chimera_vfs_get_module(thread, dir_fh, dir_fhlen);

    /* WRITE+EXECUTE on the destination directory is enforced by the engine even
     * for DELEGATES_DAC (passthrough) backends: a hard link is created by file
     * handle (linkat with AT_EMPTY_PATH / open_by_handle_at), so the kernel
     * never traverses the destination directory and never checks its write
     * permission for the caller (pjd link/07). */
    if (module && chimera_vfs_gate_needed_dac(cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
        gate->fh             = fh;
        gate->fhlen          = fhlen;
        gate->dir_fh         = dir_fh;
        gate->dir_fhlen      = dir_fhlen;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->replace        = replace;
        gate->attr_mask      = attr_mask;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        gate->callback       = callback;
        gate->private_data   = private_data;

        chimera_vfs_gate_fh_dac(thread, cred, dir_fh, dir_fhlen,
                                CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE,
                                chimera_vfs_link_at_gate_complete, gate);
        return;
    }

    chimera_vfs_link_at_dispatch(thread, cred, fh, fhlen, dir_fh, dir_fhlen,
                                 name, namelen, replace, attr_mask,
                                 pre_attr_mask, post_attr_mask, callback,
                                 private_data);
} /* chimera_vfs_link_at */
